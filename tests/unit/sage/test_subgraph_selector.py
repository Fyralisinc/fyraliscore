"""Unit tests for services.reasoning.sage.subgraph_selector (Phase 7 v1).

Pure Python — no DB, no LLM. Constructs ``ActivatedNode`` /
``CandidateEdge`` fixtures inline so this file does not depend on
Wave-2's structural gate scorer being implemented.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.reasoning.sage.structural_features.types import ModelStructuralFeatures
from services.reasoning.sage.subgraph_selector import (
    EXCLUSION_REASONS,
    ActivatedNode,
    CandidateEdge,
    ExclusionReason,
    SelectionBudget,
    SubgraphSelection,
    SubgraphSelector,
)


# ---------------------------------------------------------------------
# Fixture factories
# ---------------------------------------------------------------------


_TENANT = UUID("00000000-0000-0000-0000-000000000001")


def _sf(
    *,
    model_id: UUID,
    hub_score: float = 0.0,
    bridge_score: float = 0.0,
) -> ModelStructuralFeatures:
    return ModelStructuralFeatures(
        model_id=model_id,
        tenant_id=_TENANT,
        hub_score=hub_score,
        bridge_score=bridge_score,
    )


def _node(
    *,
    model_id: UUID | None = None,
    activation_score: float = 0.5,
    reasons: tuple[str, ...] = (),
    hub_score: float = 0.0,
    bridge_score: float = 0.0,
    with_features: bool = True,
) -> ActivatedNode:
    mid = model_id or uuid4()
    sf = (
        _sf(model_id=mid, hub_score=hub_score, bridge_score=bridge_score)
        if with_features
        else None
    )
    return ActivatedNode(
        model_id=mid,
        activation_score=activation_score,
        activation_reasons=reasons,
        structural_features=sf,
    )


def _edge(
    *,
    source: UUID,
    target: UUID,
    gate: float = 0.5,
    edge_type: str = "depends_on",
) -> CandidateEdge:
    return CandidateEdge(
        edge_id=uuid4(),
        source_model_id=source,
        target_model_id=target,
        edge_type=edge_type,
        gate_score=gate,
    )


@pytest.fixture
def selector() -> SubgraphSelector:
    return SubgraphSelector()


# ---------------------------------------------------------------------
# 1) Generic hub: summarized for DEPENDENCY, kept for CONSTRAINT.
# ---------------------------------------------------------------------


def test_generic_hub_summarized_for_dependency_but_kept_for_constraint(
    selector: SubgraphSelector,
) -> None:
    hub_id = uuid4()
    hub = _node(
        model_id=hub_id,
        activation_score=0.9,
        hub_score=0.85,           # >= 0.70 floor
        bridge_score=0.0,
        reasons=("matches:platform",),
    )

    # No incident edges => can't be saved by a high-gate bridge edge.
    dep = selector.select(
        activated_nodes=[hub],
        candidate_edges=[],
        question_primitive="DEPENDENCY",
    )
    assert hub_id not in dep.selected_nodes
    assert hub_id in dep.summarized_hubs
    assert any(
        ex.model_id == hub_id and ex.reason == "generic_hub" and ex.summarized
        for ex in dep.excluded
    )

    cons = selector.select(
        activated_nodes=[hub],
        candidate_edges=[],
        question_primitive="CONSTRAINT",
    )
    assert hub_id in cons.selected_nodes
    assert hub_id not in cons.summarized_hubs


# ---------------------------------------------------------------------
# 2) Bridge node preserved across pruning even when activation is low.
# ---------------------------------------------------------------------


def test_bridge_node_preserved_with_low_activation(
    selector: SubgraphSelector,
) -> None:
    bridge_id = uuid4()
    bridge = _node(
        model_id=bridge_id,
        activation_score=0.05,    # well below DEPENDENCY threshold
        bridge_score=0.75,        # >= 0.60 floor
        hub_score=0.0,
        reasons=(),
    )
    sel = selector.select(
        activated_nodes=[bridge],
        candidate_edges=[],
        question_primitive="DEPENDENCY",
    )
    assert bridge_id in sel.selected_nodes
    assert bridge_id in sel.bridge_nodes


# ---------------------------------------------------------------------
# 3) Counterevidence node preserved below threshold.
# ---------------------------------------------------------------------


def test_counterevidence_preserved_below_threshold(
    selector: SubgraphSelector,
) -> None:
    ce_id = uuid4()
    ce = _node(
        model_id=ce_id,
        activation_score=0.05,
        reasons=(),
    )
    sel = selector.select(
        activated_nodes=[ce],
        candidate_edges=[],
        question_primitive="DEPENDENCY",
        known_counterevidence_node_ids=(ce_id,),
    )
    assert ce_id in sel.selected_nodes
    assert sel.coverage_metrics["counterevidence_coverage"] == 1.0


# ---------------------------------------------------------------------
# 4) Required role coverage > 0 means the role-filling node is selected.
# ---------------------------------------------------------------------


def test_required_role_filler_is_selected(
    selector: SubgraphSelector,
) -> None:
    owner_id = uuid4()
    owner = _node(
        model_id=owner_id,
        activation_score=0.10,        # below OWNERSHIP threshold 0.40
        reasons=("role:owner",),
    )
    sel = selector.select(
        activated_nodes=[owner],
        candidate_edges=[],
        question_primitive="OWNERSHIP",
        required_evidence_roles=("owner",),
    )
    assert owner_id in sel.selected_nodes
    assert sel.coverage_metrics["role_coverage"] == 1.0


# ---------------------------------------------------------------------
# 5) Budget cap drops lowest-activation, never bridges or counterevidence.
# ---------------------------------------------------------------------


def test_budget_cap_preserves_bridge_and_counterevidence(
) -> None:
    # Tiny cap so we definitely hit it.
    sel = SubgraphSelector(budget=SelectionBudget(max_nodes=2, max_edges=10))

    bridge_id = uuid4()
    counter_id = uuid4()
    high_id = uuid4()
    mid_id = uuid4()
    low_id = uuid4()

    nodes = [
        _node(model_id=bridge_id, activation_score=0.05, bridge_score=0.9),
        _node(model_id=counter_id, activation_score=0.05),
        _node(model_id=high_id, activation_score=0.95),
        _node(model_id=mid_id, activation_score=0.60),
        _node(model_id=low_id, activation_score=0.35),
    ]
    out = sel.select(
        activated_nodes=nodes,
        candidate_edges=[],
        question_primitive="DEPENDENCY",
        known_counterevidence_node_ids=(counter_id,),
    )

    # Protected always present.
    assert bridge_id in out.selected_nodes
    assert counter_id in out.selected_nodes
    # Lowest-activation non-protected is dropped for budget reasons.
    assert low_id not in out.selected_nodes
    dropped_reasons = {
        ex.reason for ex in out.excluded if ex.model_id == low_id
    }
    assert "budget_exhausted" in dropped_reasons


# ---------------------------------------------------------------------
# 6) Edge cap respects max_edges.
# ---------------------------------------------------------------------


def test_edge_cap_respects_max_edges() -> None:
    sel = SubgraphSelector(
        budget=SelectionBudget(max_nodes=10, max_edges=2)
    )
    a, b, c, d = uuid4(), uuid4(), uuid4(), uuid4()
    nodes = [
        _node(model_id=a, activation_score=0.9),
        _node(model_id=b, activation_score=0.9),
        _node(model_id=c, activation_score=0.9),
        _node(model_id=d, activation_score=0.9),
    ]
    edges = [
        _edge(source=a, target=b, gate=0.9),
        _edge(source=b, target=c, gate=0.8),
        _edge(source=c, target=d, gate=0.7),
        _edge(source=a, target=d, gate=0.6),
    ]
    out = sel.select(
        activated_nodes=nodes,
        candidate_edges=edges,
        question_primitive="DEPENDENCY",
    )
    assert len(out.selected_edges) == 2


# ---------------------------------------------------------------------
# 7) Low-gate-score edge (<0.3) is excluded.
# ---------------------------------------------------------------------


def test_low_gate_edge_excluded(selector: SubgraphSelector) -> None:
    a, b = uuid4(), uuid4()
    nodes = [
        _node(model_id=a, activation_score=0.9),
        _node(model_id=b, activation_score=0.9),
    ]
    good = _edge(source=a, target=b, gate=0.50)
    bad = _edge(source=a, target=b, gate=0.10)
    out = selector.select(
        activated_nodes=nodes,
        candidate_edges=[good, bad],
        question_primitive="DEPENDENCY",
    )
    assert good.edge_id in out.selected_edges
    assert bad.edge_id not in out.selected_edges


# ---------------------------------------------------------------------
# 8) All exclusion reasons fall in the allowed enum.
# ---------------------------------------------------------------------


def test_exclusion_reasons_are_from_enum(selector: SubgraphSelector) -> None:
    hub_id = uuid4()
    stale_id = uuid4()
    oos_id = uuid4()
    low_id = uuid4()

    nodes = [
        _node(
            model_id=hub_id,
            activation_score=0.9,
            hub_score=0.85,
            reasons=("matches:platform",),
        ),
        _node(
            model_id=stale_id,
            activation_score=0.9,
            reasons=("stale",),
        ),
        _node(
            model_id=oos_id,
            activation_score=0.9,
            reasons=("outside_access_scope",),
        ),
        _node(
            model_id=low_id,
            activation_score=0.05,
            reasons=(),
        ),
    ]
    out = selector.select(
        activated_nodes=nodes,
        candidate_edges=[],
        question_primitive="DEPENDENCY",
    )
    assert out.excluded, "expected at least one exclusion"
    for ex in out.excluded:
        assert ex.reason in EXCLUSION_REASONS


# ---------------------------------------------------------------------
# 9) coverage_metrics are in [0, 1].
# ---------------------------------------------------------------------


def test_coverage_metrics_in_unit_interval(selector: SubgraphSelector) -> None:
    bridge_id = uuid4()
    counter_id = uuid4()
    owner_id = uuid4()

    nodes = [
        _node(model_id=bridge_id, activation_score=0.9, bridge_score=0.9),
        _node(model_id=counter_id, activation_score=0.9),
        _node(
            model_id=owner_id,
            activation_score=0.9,
            reasons=("role:owner",),
        ),
        # A second bridge that we'll NOT select because we'll force it
        # out via a generic-hub overlay — actually we just want a partial
        # bridge_coverage scenario. Easier: add a low-activation bridge
        # but make it also generic_hub-eligible... keep simple: add an
        # unselected bridge via budget cap.
        _node(
            model_id=uuid4(),
            activation_score=0.05,
            bridge_score=0.0,
            reasons=("stale",),  # will be dropped
        ),
    ]
    out = selector.select(
        activated_nodes=nodes,
        candidate_edges=[],
        question_primitive="DEPENDENCY",
        known_counterevidence_node_ids=(counter_id,),
        required_evidence_roles=("owner",),
    )
    for k, v in out.coverage_metrics.items():
        assert 0.0 <= v <= 1.0, f"{k}={v} out of [0,1]"
    # Sanity: all three categories are covered here.
    assert out.coverage_metrics["bridge_coverage"] == 1.0
    assert out.coverage_metrics["counterevidence_coverage"] == 1.0
    assert out.coverage_metrics["role_coverage"] == 1.0


# ---------------------------------------------------------------------
# 10) All exclusions have either summarized=True or a non-"summarized" reason.
# ---------------------------------------------------------------------


def test_excluded_summarized_invariant(selector: SubgraphSelector) -> None:
    hub_id = uuid4()
    stale_id = uuid4()
    nodes = [
        _node(
            model_id=hub_id,
            activation_score=0.9,
            hub_score=0.85,
            reasons=("matches:platform",),
        ),
        _node(model_id=stale_id, activation_score=0.9, reasons=("stale",)),
    ]
    out = selector.select(
        activated_nodes=nodes,
        candidate_edges=[],
        question_primitive="DEPENDENCY",
    )
    assert out.excluded
    for ex in out.excluded:
        assert isinstance(ex, ExclusionReason)
        # Reason is never literally the string "summarized" — that's a
        # flag, not a reason. The flag distinguishes "rolled up into a
        # hub summary" from "silently dropped".
        assert ex.reason != "summarized"
        # The closed enum invariant from the spec: every reason must be
        # one of the allowed values, regardless of the summarized flag.
        assert ex.reason in EXCLUSION_REASONS


# ---------------------------------------------------------------------
# 11) Bonus: redundant local confirmations are summarized, not dropped.
# ---------------------------------------------------------------------


def test_redundant_local_confirmations_are_summarized(
    selector: SubgraphSelector,
) -> None:
    a, b = uuid4(), uuid4()
    # Two nodes with identical reason sets — b is the lower-scoring
    # twin and should be rolled up.
    reasons = ("matches:SSO", "neighbor_of:Acme")
    nodes = [
        _node(model_id=a, activation_score=0.9, reasons=reasons),
        _node(model_id=b, activation_score=0.6, reasons=reasons),
    ]
    out = selector.select(
        activated_nodes=nodes,
        candidate_edges=[],
        question_primitive="DEPENDENCY",
    )
    assert a in out.selected_nodes
    assert b not in out.selected_nodes
    assert any(
        ex.model_id == b
        and ex.reason == "redundant_local_confirmation"
        and ex.summarized
        for ex in out.excluded
    )


# ---------------------------------------------------------------------
# 12) Sanity: select() always returns a SubgraphSelection with tuples.
# ---------------------------------------------------------------------


def test_return_shape(selector: SubgraphSelector) -> None:
    out = selector.select(
        activated_nodes=[],
        candidate_edges=[],
        question_primitive="DEPENDENCY",
    )
    assert isinstance(out, SubgraphSelection)
    assert out.selected_nodes == ()
    assert out.selected_edges == ()
    assert out.bridge_nodes == ()
    assert out.summarized_hubs == ()
    assert out.excluded == ()
    assert set(out.coverage_metrics.keys()) == {
        "bridge_coverage",
        "counterevidence_coverage",
        "role_coverage",
    }
