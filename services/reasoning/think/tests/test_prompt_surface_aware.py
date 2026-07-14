from __future__ import annotations

from types import SimpleNamespace

from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.prompt import (
    build_prompt,
    prompt_static_size_report,
    select_prompt_surface,
)


def _t1(**kwargs) -> TriggerContext:
    payload = {
        "kind": "T1",
        "subkind": "event_arrival",
        "tenant_id": uuid7(),
        "observation_id": uuid7(),
    }
    payload.update(kwargs)
    return TriggerContext(**payload)


def test_surface_prompt_is_flag_guarded_by_default(monkeypatch):
    monkeypatch.delenv("THINK_SURFACE_AWARE_PROMPT", raising=False)

    prompt = build_prompt(_t1(), ContextBundle(), claims_only=True)

    assert "<prompt_surface>" not in prompt.user
    assert "surface-aware prompt" not in prompt.system.lower()
    assert "This compact pass can only emit" in prompt.system


def test_surface_claims_only_prompt_is_small_and_keeps_safety_contract(monkeypatch):
    monkeypatch.setenv("THINK_SURFACE_AWARE_PROMPT", "1")

    prompt = build_prompt(_t1(), ContextBundle(), claims_only=True)
    report = prompt_static_size_report()

    assert "<prompt_surface>" in prompt.user
    assert "schema: claims_only" in prompt.user
    assert "packs: model_memory" in prompt.user
    assert len(prompt.system) <= report["baseline_claims_only"]["chars"] * 0.65
    assert "The LLM proposes; validators constrain; appliers mutate" in prompt.system
    assert "Observations are immutable evidence" in prompt.system
    assert "Do not invent UUIDs" in prompt.system
    assert "Empty diffs are valid" in prompt.system
    assert "Surface pack: Model memory and claim formation." in prompt.system
    assert "Surface pack: Model graph" not in prompt.system
    assert "Surface pack: Acts" not in prompt.system
    assert "Do not emit edge_ops, act_ops, resource_ops" in prompt.system


def test_surface_prompt_size_report_sets_objective_reduction_floor():
    report = prompt_static_size_report()
    baseline_full = report["baseline_full"]["chars"]
    baseline_claims = report["baseline_claims_only"]["chars"]

    assert report["surface_claims_only"]["chars"] <= baseline_claims * 0.65
    assert report["surface_full_model_graph"]["chars"] <= baseline_full * 0.55
    assert report["surface_full_all_packs"]["chars"] <= baseline_full * 0.70


def test_surface_selector_routes_graph_acts_resources_and_batch(monkeypatch):
    monkeypatch.setenv("THINK_SURFACE_AWARE_PROMPT", "1")
    graph_id = str(uuid7())
    trigger = _t1(
        subkind="event_batch",
        observation_ids=[uuid7(), uuid7()],
        seed_natural_text=(
            "I've started rebuilding the capacity budget tracker and the "
            "resource allocation is blocked."
        ),
    )
    bundle = ContextBundle(
        acts_summary={
            "goals": [],
            "commitments": [
                SimpleNamespace(
                    id=uuid7(),
                    state="active",
                    owner_id=uuid7(),
                    due_date=None,
                    title="Rebuild capacity budget tracker",
                )
            ],
            "decisions": [],
        },
        resources_summary=[
            SimpleNamespace(
                id=uuid7(),
                kind="budget",
                identity="capacity budget tracker",
                description="Resource allocation tracker",
                utilization_state="constrained",
                current_value={"budget": "tight"},
            )
        ],
        notes={
            "model_selection": {
                "selected_model_ids": [graph_id],
                "pathway_survival": {"G": {"selected_model_ids": [graph_id]}},
            }
        },
    )

    surface = select_prompt_surface(trigger, bundle, claims_only=False)
    prompt = build_prompt(trigger, bundle, claims_only=False)

    assert surface.packs == (
        "model_memory",
        "batch",
        "graph",
        "acts",
        "resources",
    )
    assert "packs: model_memory, batch, graph, acts, resources" in prompt.user
    assert "Surface pack: Batch compression." in prompt.system
    assert "Surface pack: Model graph and relationship topology." in prompt.system
    assert "Surface pack: Acts and recommendations." in prompt.system
    assert "Surface pack: Resources." in prompt.system
    assert "capacity budget tracker" not in prompt.system


def test_surface_selector_routes_topology_candidate_without_acts(monkeypatch):
    monkeypatch.setenv("THINK_SURFACE_AWARE_PROMPT", "1")
    trigger = TriggerContext(
        kind="T4",
        subkind="latent_relationship_candidate",
        tenant_id=uuid7(),
        seed_signature={
            "relationship_candidate": {
                "id": "candidate-1",
                "member_model_ids": [str(uuid7()), str(uuid7())],
                "explanation": "Two delivery risks share a decision bottleneck.",
            }
        },
    )

    surface = select_prompt_surface(trigger, ContextBundle(), claims_only=False)
    prompt = build_prompt(trigger, ContextBundle(), claims_only=False)

    assert "lifecycle" in surface.packs
    assert "graph" in surface.packs
    assert "topology_candidate" in surface.packs
    assert "acts" not in surface.packs
    assert "Surface pack: Topology and pattern candidates." in prompt.system
    assert "Surface pack: Acts and recommendations." not in prompt.system


def test_surface_prompt_keeps_system_cache_bucket_stable(monkeypatch):
    monkeypatch.setenv("THINK_SURFACE_AWARE_PROMPT", "1")

    a = build_prompt(
        _t1(seed_signature={"source_channel": "slack"}),
        ContextBundle(),
        triggering_content="alpha",
        claims_only=True,
    )
    b = build_prompt(
        _t1(seed_signature={"source_channel": "email"}),
        ContextBundle(),
        triggering_content="beta",
        claims_only=True,
    )

    assert a.system == b.system
    assert "alpha" not in a.system
    assert "beta" not in b.system
    assert "alpha" in a.user
    assert "beta" in b.user
