"""Unit tests for the T6 trigger surface in services.think.prompt.

Verifies:
  - <topology_context> section is rendered when bundle.topology_context
    is set, with neighborhood + recent_phase_events lines.
  - T6-specific operating instructions are appended.
  - is_authoritative(T6) returns False (so T6 routes to LLM).
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext
from services.think.deterministic import is_authoritative
from services.think.prompt import build_prompt


def _empty_bundle(topology_context=None) -> ContextBundle:
    return ContextBundle(
        observations=[],
        models=[],
        acts_summary={"goals": [], "commitments": [], "decisions": []},
        resources_summary=[],
        bridge_context=None,
        topology_context=topology_context,
    )


def test_t6_is_not_authoritative():
    trigger = TriggerContext(
        kind="T6",
        tenant_id=uuid4(),
    )
    assert is_authoritative(trigger) is False


def test_t6_instructions_appended():
    trigger = TriggerContext(
        kind="T6",
        tenant_id=uuid4(),
        topology_event_kind="emergence",
        neighborhood_id=uuid4(),
    )
    pair = build_prompt(trigger, _empty_bundle())
    user = pair.user
    assert "T6 trigger" in user
    assert "TOPOLOGY phase event" in user
    assert "<topology_context>" in user
    # The instructions reference recommendation surfacing for emergence
    # / merge / high-magnitude split.
    assert "recommendation" in user


def test_topology_context_section_rendered_with_neighborhoods():
    seed_n = uuid4()
    other_n = uuid4()
    topo_ctx = {
        "seed_neighborhood_id": seed_n,
        "neighborhoods": [
            {
                "id": seed_n,
                "named_signature": "engineering velocity",
                "density": 0.42,
                "member_count": 6,
                "matched_in_bundle": 3,
                "centrality_top_member_id": uuid4(),
                "status": "active",
                "last_recomputed_at": datetime.now(timezone.utc),
                "is_seed": True,
            },
            {
                "id": other_n,
                "named_signature": "customer commitments",
                "density": 0.18,
                "member_count": 4,
                "matched_in_bundle": 1,
                "centrality_top_member_id": uuid4(),
                "status": "active",
                "last_recomputed_at": datetime.now(timezone.utc),
                "is_seed": False,
            },
        ],
        "recent_phase_events": [
            {
                "id": uuid4(),
                "kind": "emergence",
                "occurred_at": datetime.now(timezone.utc),
                "neighborhood_id": seed_n,
                "named_signature": "engineering velocity",
                "magnitude": 6.0,
            },
        ],
    }
    bundle = _empty_bundle(topology_context=topo_ctx)
    trigger = TriggerContext(kind="T1", tenant_id=uuid4())
    user = build_prompt(trigger, bundle).user
    assert "engineering velocity" in user
    assert "customer commitments" in user
    assert " (SEED)" in user
    assert "recent_phase_events" in user
    assert "kind=emergence" in user


def test_topology_context_section_renders_empty_marker_when_none():
    bundle = _empty_bundle(topology_context=None)
    trigger = TriggerContext(kind="T1", tenant_id=uuid4())
    user = build_prompt(trigger, bundle).user
    assert "[no neighborhood context for this trigger]" in user


def test_topology_context_handles_no_density():
    topo_ctx = {
        "seed_neighborhood_id": None,
        "neighborhoods": [
            {
                "id": uuid4(),
                "named_signature": None,
                "density": None,
                "member_count": 2,
                "matched_in_bundle": 1,
                "centrality_top_member_id": None,
                "status": "active",
                "last_recomputed_at": datetime.now(timezone.utc),
                "is_seed": False,
            },
        ],
        "recent_phase_events": [],
    }
    bundle = _empty_bundle(topology_context=topo_ctx)
    trigger = TriggerContext(kind="T1", tenant_id=uuid4())
    user = build_prompt(trigger, bundle).user
    # density=n/a fallback rendered without crashing.
    assert "density=n/a" in user
    assert "[unnamed]" in user
