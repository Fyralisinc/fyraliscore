"""services.sage.subgraph_selector — Phase 7: Query-Conditioned Subgraph Selector.

Spec: fyralis-sage-synthesis-self-evolution.md §7.6 (Stage E) and Phase 7
(§1685-1750). Pure-Python, deterministic v1. No DB, no LLM.

Given a set of already-activated Nodes/Models, a set of candidate edges
(each carrying a precomputed gate score from Phase 6 / Stage D), and a
``question_primitive`` ("DEPENDENCY", "OWNERSHIP", "CONTRADICTION",
"CONSTRAINT", ...), produce a compact ``SubgraphSelection`` that:

  * Keeps Nodes that answer the question, bridge regions, provide
    counterevidence, or fill a required evidence role.
  * Drops or rolls up Nodes that are generic hubs, redundant local
    confirmations, stale, or low-trust unsupported.
  * Respects node/edge budget caps without ever evicting bridge or
    counterevidence Nodes.
  * Reports coverage metrics (bridge / counterevidence / required-role)
    so downstream synthesis can detect under-coverage.

The selector is decoupled from the actual structural gate scorer
(Phase 6): each ``CandidateEdge`` carries its ``gate_score`` already.
This keeps the selector trivially unit-testable and lets Wave 2's
``StructuralGateScorer`` evolve independently.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from services.sage.structural_features.types import ModelStructuralFeatures


# ---------------------------------------------------------------------
# Public type surface
# ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ActivatedNode:
    """A Node/Model that survived Stage C activation+propagation.

    ``activation_score`` is the propagated score in [0, 1].
    ``activation_reasons`` is a tuple of short tags (e.g.
    ``("matches:SSO", "neighbor_of:Acme")``) used to detect redundant
    local confirmations. ``structural_features`` is optional but, when
    present, lets the selector apply hub/bridge regularization.
    """

    model_id: UUID
    activation_score: float
    activation_reasons: tuple[str, ...]
    structural_features: Optional[ModelStructuralFeatures]


@dataclass(frozen=True, slots=True)
class CandidateEdge:
    """An edge whose endpoints may or may not survive selection.

    ``gate_score`` is the precomputed Phase 6 / Stage D gate output in
    [0, 1]. Edges with a gate score below the floor (0.3) are dropped
    even if both endpoints survive.
    """

    edge_id: UUID
    source_model_id: UUID
    target_model_id: UUID
    edge_type: str
    gate_score: float


@dataclass(frozen=True, slots=True)
class SelectionBudget:
    """Hard caps applied after rule-based selection."""

    max_nodes: int = 80
    max_edges: int = 120
    max_summarized_hubs: int = 10


# Allowed exclusion reasons (kept as a frozenset so tests can assert
# the closed enum without importing a separate Literal helper).
EXCLUSION_REASONS: frozenset[str] = frozenset({
    "generic_hub",
    "redundant_local_confirmation",
    "stale",
    "low_trust_unsupported",
    "outside_access_scope",
    "budget_exhausted",
})


@dataclass(frozen=True, slots=True)
class ExclusionReason:
    """Why a candidate Node was dropped or summarized.

    ``summarized=True`` means the Node was rolled into a hub summary
    rather than being silently dropped; the synthesis layer can still
    surface it as "+N similar generic platforms".
    """

    model_id: UUID
    reason: str
    summarized: bool


@dataclass(frozen=True, slots=True)
class SubgraphSelection:
    """Final subgraph + coverage diagnostics."""

    selected_nodes: tuple[UUID, ...]
    selected_edges: tuple[UUID, ...]
    bridge_nodes: tuple[UUID, ...]
    summarized_hubs: tuple[UUID, ...]
    excluded: tuple[ExclusionReason, ...]
    coverage_metrics: dict[str, float]


# ---------------------------------------------------------------------
# Tuning constants (v1 — intentionally readable + overridable later)
# ---------------------------------------------------------------------


_ACTIVATION_THRESHOLDS: dict[str, float] = {
    "DEPENDENCY":     0.30,
    "OWNERSHIP":      0.40,
    "CONTRADICTION":  0.25,
}
_DEFAULT_ACTIVATION_THRESHOLD: float = 0.30

# Bridge nodes are preserved across pruning even when their activation
# is below the question-primitive threshold (doc §7.6 "connect two
# otherwise separate regions").
_BRIDGE_SCORE_FLOOR: float = 0.60

# Generic-hub regularization. For most question primitives a Node with
# ``hub_score >= 0.7`` is rolled up into a summary unless it provides
# counterevidence or sits on a high-gate bridge edge. CONSTRAINT-type
# questions are different — they often want the bottleneck hub itself
# — so we exempt them.
_HUB_SCORE_FLOOR: float = 0.70
_HUB_EXEMPT_PRIMITIVES: frozenset[str] = frozenset({"CONSTRAINT"})

# Edges with a gate score below this floor are dropped even if both
# endpoints are selected. Mirrors the Phase 6 gate threshold in the
# spec (§7.5 — suppresses generic hubs through edges, not just nodes).
_GATE_SCORE_FLOOR: float = 0.30

# Bridge-edge floor used when deciding whether a high-hub Node should
# be kept anyway because it sits on a meaningful inter-region edge.
_BRIDGE_EDGE_GATE_FLOOR: float = 0.60

# Redundant-local-confirmation detector: if two activated Nodes share
# at least this fraction of their ``activation_reasons``, the
# lower-scoring one is rolled up.
_REDUNDANT_REASON_OVERLAP: float = 0.80

# Staleness: an activated Node may carry a structural-features row
# whose ``updated_at`` is None. We treat presence of an explicit
# ``"stale"`` reason tag as the v1 signal — no clocks here.
_STALE_REASON_TAGS: frozenset[str] = frozenset({"stale", "is_stale"})

# Outside-access-scope: same convention — caller annotates the Node
# via an explicit reason tag.
_OUT_OF_SCOPE_REASON_TAGS: frozenset[str] = frozenset({
    "outside_access_scope",
    "out_of_scope",
})


# ---------------------------------------------------------------------
# The selector
# ---------------------------------------------------------------------


class SubgraphSelector:
    """Pure-Python query-conditioned subgraph selector.

    Stateless. Safe to share across requests / event loops. The default
    ``SelectionBudget`` matches the doc (§7.6 budget constraints).
    """

    def __init__(self, *, budget: SelectionBudget | None = None) -> None:
        self._budget: SelectionBudget = budget or SelectionBudget()

    # ------------------------------------------------------------- public

    def select(
        self,
        *,
        activated_nodes: list[ActivatedNode],
        candidate_edges: list[CandidateEdge],
        question_primitive: str,
        required_evidence_roles: tuple[str, ...] = (),
        known_counterevidence_node_ids: tuple[UUID, ...] = (),
    ) -> SubgraphSelection:
        primitive = (question_primitive or "").strip().upper()
        threshold = _ACTIVATION_THRESHOLDS.get(
            primitive, _DEFAULT_ACTIVATION_THRESHOLD
        )
        counter_set: frozenset[UUID] = frozenset(known_counterevidence_node_ids)
        required_roles: tuple[str, ...] = tuple(required_evidence_roles)

        # ---- pre-compute per-node features we need below --------------
        gate_by_endpoint = _gate_scores_by_endpoint(candidate_edges)

        bridge_nodes: set[UUID] = set()
        keep: set[UUID] = set()
        excluded: list[ExclusionReason] = []
        summarized_hubs: list[UUID] = []
        role_covered: set[str] = set()

        # Bridge candidates first — we need them resolved before we can
        # ask "is this hub justified by a bridge edge?".
        for n in activated_nodes:
            sf = n.structural_features
            if sf is not None and (sf.bridge_score or 0.0) >= _BRIDGE_SCORE_FLOOR:
                bridge_nodes.add(n.model_id)

        # ---- per-node keep/drop/summarize decisions -------------------
        # Sort deterministically: highest activation first, then UUID
        # for stable tie-breaks (important for the budget-cap rule).
        ordered = sorted(
            activated_nodes,
            key=lambda x: (-x.activation_score, str(x.model_id)),
        )

        # Track previously-kept nodes' reason-sets so we can detect
        # redundant local confirmations.
        kept_reason_sets: list[tuple[UUID, frozenset[str]]] = []

        for n in ordered:
            mid = n.model_id
            reasons = n.activation_reasons or ()
            reason_set = frozenset(reasons)
            sf = n.structural_features

            # --- preserved-no-matter-what categories -------------------
            is_bridge = mid in bridge_nodes
            is_counter = mid in counter_set
            role_match = _role_match(reasons, required_roles)

            # --- explicit caller-tagged drops --------------------------
            if reason_set & _OUT_OF_SCOPE_REASON_TAGS:
                excluded.append(ExclusionReason(mid, "outside_access_scope", False))
                continue
            if reason_set & _STALE_REASON_TAGS and not (is_bridge or is_counter):
                excluded.append(ExclusionReason(mid, "stale", False))
                continue

            # --- generic-hub regularization ----------------------------
            hub_score = (sf.hub_score or 0.0) if sf is not None else 0.0
            if (
                hub_score >= _HUB_SCORE_FLOOR
                and not is_counter
                and not is_bridge
                and primitive not in _HUB_EXEMPT_PRIMITIVES
                and not _has_high_gate_bridge_edge(mid, gate_by_endpoint)
            ):
                if len(summarized_hubs) < self._budget.max_summarized_hubs:
                    summarized_hubs.append(mid)
                excluded.append(ExclusionReason(mid, "generic_hub", True))
                continue

            # --- redundant local confirmations -------------------------
            # A Node is redundant if its reason-set overlaps an
            # already-kept Node's reason-set by >= the configured
            # fraction. We only drop the lower-scoring one — and since
            # we iterate highest-first, that is always "this" Node.
            if not (is_bridge or is_counter or role_match) and _is_redundant(
                reason_set, kept_reason_sets, _REDUNDANT_REASON_OVERLAP
            ):
                excluded.append(
                    ExclusionReason(mid, "redundant_local_confirmation", True)
                )
                continue

            # --- low-trust unsupported ---------------------------------
            # "Low trust unsupported" = activation below threshold AND
            # no inbound high-gate edge supporting it AND not a bridge
            # / counterevidence / role-filler.
            below_threshold = n.activation_score < threshold
            if below_threshold and not (is_bridge or is_counter or role_match):
                if not _has_inbound_high_gate_edge(mid, candidate_edges):
                    excluded.append(
                        ExclusionReason(mid, "low_trust_unsupported", False)
                    )
                    continue

            # --- keep -------------------------------------------------
            keep.add(mid)
            kept_reason_sets.append((mid, reason_set))
            if role_match:
                role_covered.add(role_match)

        # ---- budget cap on nodes -------------------------------------
        # Drop lowest-activation tail, but never bridge or
        # counterevidence Nodes. Required-role fillers are also
        # protected to keep ``role_coverage`` meaningful.
        if len(keep) > self._budget.max_nodes:
            keep, dropped_for_budget = _apply_node_budget(
                keep=keep,
                ordered=ordered,
                protected=bridge_nodes | counter_set,
                required_roles=required_roles,
                max_nodes=self._budget.max_nodes,
            )
            for mid in dropped_for_budget:
                excluded.append(ExclusionReason(mid, "budget_exhausted", False))

        # ---- edge selection ------------------------------------------
        selected_edges: list[UUID] = []
        for e in candidate_edges:
            if e.gate_score < _GATE_SCORE_FLOOR:
                continue
            if e.source_model_id not in keep or e.target_model_id not in keep:
                continue
            selected_edges.append(e.edge_id)
        # Edge cap: drop lowest gate-score tail. Build an index back to
        # the CandidateEdge so we can sort by gate score, then truncate.
        if len(selected_edges) > self._budget.max_edges:
            edge_by_id = {e.edge_id: e for e in candidate_edges}
            selected_edges.sort(
                key=lambda eid: (-edge_by_id[eid].gate_score, str(eid))
            )
            selected_edges = selected_edges[: self._budget.max_edges]

        # ---- coverage metrics ----------------------------------------
        coverage = _coverage_metrics(
            bridge_nodes_total=bridge_nodes,
            bridge_nodes_kept=bridge_nodes & keep,
            counter_total=counter_set,
            counter_kept=counter_set & keep,
            required_roles=required_roles,
            covered_roles=role_covered,
        )

        # Stable ordering of outputs for deterministic tests.
        selected_nodes = tuple(sorted(keep, key=str))
        selected_edges_t = tuple(sorted(selected_edges, key=str))
        bridge_nodes_t = tuple(sorted(bridge_nodes & keep, key=str))
        summarized_t = tuple(summarized_hubs)
        excluded_t = tuple(excluded)

        return SubgraphSelection(
            selected_nodes=selected_nodes,
            selected_edges=selected_edges_t,
            bridge_nodes=bridge_nodes_t,
            summarized_hubs=summarized_t,
            excluded=excluded_t,
            coverage_metrics=coverage,
        )


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------


def _gate_scores_by_endpoint(
    edges: list[CandidateEdge],
) -> dict[UUID, list[float]]:
    """Map each endpoint model_id to the list of incident edge gate
    scores. Used to decide whether a high-hub Node sits on a
    "high-gate-score bridge edge" and therefore deserves to be kept.
    """
    out: dict[UUID, list[float]] = {}
    for e in edges:
        out.setdefault(e.source_model_id, []).append(e.gate_score)
        out.setdefault(e.target_model_id, []).append(e.gate_score)
    return out


def _has_high_gate_bridge_edge(
    model_id: UUID, gate_by_endpoint: dict[UUID, list[float]]
) -> bool:
    scores = gate_by_endpoint.get(model_id, ())
    return any(s >= _BRIDGE_EDGE_GATE_FLOOR for s in scores)


def _has_inbound_high_gate_edge(
    model_id: UUID, edges: list[CandidateEdge]
) -> bool:
    """True if any edge targets ``model_id`` with gate >= floor.

    Used by the "low-trust unsupported" rule: a sub-threshold Node may
    still be kept if some neighbor's relationship to it has survived
    the structural gate.
    """
    for e in edges:
        if e.target_model_id == model_id and e.gate_score >= _GATE_SCORE_FLOOR:
            return True
    return False


def _role_match(
    reasons: tuple[str, ...], required_roles: tuple[str, ...]
) -> Optional[str]:
    """Return the first required role this Node fills, or None.

    Convention: an activation reason of the form ``"role:<name>"``
    means "this Node provides evidence role <name>".
    """
    if not required_roles:
        return None
    role_tags = {
        r.split(":", 1)[1] for r in reasons if r.startswith("role:")
    }
    for needed in required_roles:
        if needed in role_tags:
            return needed
    return None


def _is_redundant(
    candidate_reasons: frozenset[str],
    kept_reason_sets: list[tuple[UUID, frozenset[str]]],
    overlap_threshold: float,
) -> bool:
    if not candidate_reasons:
        return False
    for _, kept in kept_reason_sets:
        if not kept:
            continue
        inter = len(candidate_reasons & kept)
        denom = min(len(candidate_reasons), len(kept))
        if denom == 0:
            continue
        if (inter / denom) >= overlap_threshold:
            return True
    return False


def _apply_node_budget(
    *,
    keep: set[UUID],
    ordered: list[ActivatedNode],
    protected: frozenset[UUID] | set[UUID],
    required_roles: tuple[str, ...],
    max_nodes: int,
) -> tuple[set[UUID], list[UUID]]:
    """Trim ``keep`` to ``max_nodes`` by dropping the lowest-activation
    tail. Protected and role-filling Nodes are never dropped.
    """
    # Identify role-filling Nodes already in keep — also protected.
    role_protected: set[UUID] = set()
    if required_roles:
        for n in ordered:
            if n.model_id not in keep:
                continue
            if _role_match(n.activation_reasons or (), required_roles):
                role_protected.add(n.model_id)
    fully_protected: set[UUID] = set(protected) | role_protected

    # Walk ordered (highest-first); keep protected + top-N non-protected
    # until we hit the cap.
    kept: set[UUID] = set()
    dropped: list[UUID] = []
    slots_left = max_nodes
    # Reserve slots for protected first so we always honour them.
    for n in ordered:
        if n.model_id in fully_protected and n.model_id in keep:
            kept.add(n.model_id)
            slots_left -= 1
    for n in ordered:
        mid = n.model_id
        if mid not in keep or mid in kept:
            continue
        if slots_left > 0:
            kept.add(mid)
            slots_left -= 1
        else:
            dropped.append(mid)
    # If we somehow over-reserved (protected > max_nodes) we still
    # honour protections — the doc explicitly forbids dropping them.
    return kept, dropped


def _coverage_metrics(
    *,
    bridge_nodes_total: set[UUID],
    bridge_nodes_kept: set[UUID],
    counter_total: frozenset[UUID],
    counter_kept: frozenset[UUID] | set[UUID],
    required_roles: tuple[str, ...],
    covered_roles: set[str],
) -> dict[str, float]:
    """Three coverage scalars in [0, 1].

    Conventions:
      * no bridges available    -> bridge_coverage = 0.0
      * no counterevidence req. -> counterevidence_coverage = 1.0
      * no required roles       -> role_coverage = 1.0
    """
    bridge_cov = (
        len(bridge_nodes_kept) / len(bridge_nodes_total)
        if bridge_nodes_total
        else 0.0
    )
    counter_cov = (
        len(counter_kept) / len(counter_total) if counter_total else 1.0
    )
    role_cov = (
        len(covered_roles) / len(required_roles) if required_roles else 1.0
    )
    return {
        "bridge_coverage": _clamp01(bridge_cov),
        "counterevidence_coverage": _clamp01(counter_cov),
        "role_coverage": _clamp01(role_cov),
    }


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


__all__ = [
    "ActivatedNode",
    "CandidateEdge",
    "SelectionBudget",
    "ExclusionReason",
    "SubgraphSelection",
    "SubgraphSelector",
    "EXCLUSION_REASONS",
]
