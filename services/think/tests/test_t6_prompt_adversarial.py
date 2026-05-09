"""Prompt-rendering adversarials for the S3 <topology_context>
section + T6 instruction injection. Targets unicode, very large
contexts, malformed shapes that the assembler might produce, and
the relocate-doc presence in the system prompt."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext
from services.think.prompt import build_prompt


def _bundle(topology_context=None) -> ContextBundle:
    return ContextBundle(
        observations=[],
        models=[],
        acts_summary={"goals": [], "commitments": [], "decisions": []},
        resources_summary=[],
        bridge_context=None,
        topology_context=topology_context,
    )


def test_relocate_documented_in_system_prompt():
    """The system prompt MUST mention the relocate op shape so the
    LLM knows it can emit one."""
    pair = build_prompt(TriggerContext(kind="T1", tenant_id=uuid4()), _bundle())
    assert "claim_ops.relocate" in pair.system or "relocate" in pair.system
    assert "relocate_target" in pair.system


def test_topology_context_handles_huge_neighborhood_list():
    """50 neighborhoods in topology_context shouldn't crash the
    renderer (pruning happens upstream in the assembler, but the
    prompt still iterates)."""
    big_topo = {
        "seed_neighborhood_id": None,
        "neighborhoods": [
            {
                "id": uuid4(),
                "named_signature": f"cluster {i}",
                "density": 0.5,
                "member_count": 5,
                "matched_in_bundle": 1,
                "centrality_top_member_id": None,
                "status": "active",
                "last_recomputed_at": datetime.now(timezone.utc),
                "is_seed": False,
            }
            for i in range(50)
        ],
        "recent_phase_events": [],
    }
    pair = build_prompt(
        TriggerContext(kind="T1", tenant_id=uuid4()),
        _bundle(topology_context=big_topo),
    )
    assert pair.user.count("neighborhood id=") == 50


def test_topology_context_handles_unicode_named_signature():
    topo = {
        "seed_neighborhood_id": None,
        "neighborhoods": [
            {
                "id": uuid4(),
                "named_signature": "東京 / Carmen 🔥",
                "density": 0.5,
                "member_count": 3,
                "matched_in_bundle": 2,
                "centrality_top_member_id": None,
                "status": "active",
                "last_recomputed_at": datetime.now(timezone.utc),
                "is_seed": False,
            }
        ],
        "recent_phase_events": [],
    }
    user = build_prompt(
        TriggerContext(kind="T1", tenant_id=uuid4()),
        _bundle(topology_context=topo),
    ).user
    assert "東京" in user
    assert "🔥" in user


def test_topology_context_handles_named_signature_none_in_event():
    """A topology_event with named_signature=None should render
    [unnamed] without crashing."""
    topo = {
        "seed_neighborhood_id": uuid4(),
        "neighborhoods": [],
        "recent_phase_events": [
            {
                "id": uuid4(),
                "kind": "drift",
                "occurred_at": datetime.now(timezone.utc),
                "neighborhood_id": uuid4(),
                "named_signature": None,
                "magnitude": 0.5,
            }
        ],
    }
    user = build_prompt(
        TriggerContext(kind="T1", tenant_id=uuid4()),
        _bundle(topology_context=topo),
    ).user
    assert "[unnamed]" in user


def test_topology_context_handles_null_density_and_no_members():
    """If a neighborhood somehow has density=None and member_count=0,
    rendering should gracefully degrade."""
    topo = {
        "seed_neighborhood_id": None,
        "neighborhoods": [
            {
                "id": uuid4(),
                "named_signature": "edge case",
                "density": None,
                "member_count": 0,
                "matched_in_bundle": 0,
                "centrality_top_member_id": None,
                "status": "active",
                "last_recomputed_at": datetime.now(timezone.utc),
                "is_seed": False,
            }
        ],
        "recent_phase_events": [],
    }
    user = build_prompt(
        TriggerContext(kind="T1", tenant_id=uuid4()),
        _bundle(topology_context=topo),
    ).user
    assert "density=n/a" in user
    assert "members=0" in user


def test_topology_context_with_dict_keys_missing_logs_no_crash():
    """A topology_context with sparse fields should not crash render.
    (Defensive — protect against assembler regressions.)"""
    topo = {
        "neighborhoods": [
            {
                # Missing many keys.
                "id": uuid4(),
            }
        ],
        "recent_phase_events": [],
    }
    user = build_prompt(
        TriggerContext(kind="T1", tenant_id=uuid4()),
        _bundle(topology_context=topo),
    ).user
    assert "<topology_context>" in user


def test_t6_instruction_block_includes_kind_specific_guidance():
    user = build_prompt(
        TriggerContext(
            kind="T6",
            tenant_id=uuid4(),
            topology_event_kind="emergence",
            neighborhood_id=uuid4(),
        ),
        _bundle(),
    ).user
    assert "TOPOLOGY phase event" in user
    assert "Naming the neighborhood" in user
    assert "Surfacing the shift to the CEO" in user
    assert "No-op" in user
    assert "STRICT CONSTRAINTS" in user
