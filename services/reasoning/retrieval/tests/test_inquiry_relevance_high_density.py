from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.types import ModelCreate
from services.platform.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.domain.models.repo import ModelsRepo
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


async def _add_model(
    repo: ModelsRepo,
    conn,
    *,
    tenant_id,
    born_from_event_id,
    natural: str,
    scope_entities: list[dict],
    scope_actors: list,
    kind: str = "concern",
):
    proposition = (
        {
            "kind": "state",
            "subject": natural[:60],
            "assertion": natural,
        }
        if kind == "state"
        else {
            "kind": "concern",
            "about": natural[:80],
            "nature": "risk",
            "raised_by": "high-density-test",
        }
    )
    return await repo.insert(
        ModelCreate(
            tenant_id=tenant_id,
            born_from_event_id=born_from_event_id,
            proposition=proposition,
            natural=natural,
            embedding=make_embedding(natural),
            scope_actors=scope_actors,
            scope_entities=scope_entities,
            scope_temporal={"type": "now"},
            confidence=0.62,
            confidence_at_assertion=0.62,
        ),
        conn=conn,
    )


async def test_inquiry_relevance_gate_on_rich_high_density_model_universe(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_actors=14,
        n_goals=28,
        n_commitments=90,
        n_observations=260,
        n_models=430,
        n_customers=14,
        n_decisions=12,
    )
    repo = ModelsRepo(fresh_db, embedder=None, run_topology_on_insert=False)
    hero_scope = [
        {"type": "commitment", "id": str(fs.hero_commitment_id)},
        {"type": "goal", "id": str(fs.hero_goal_id)},
        {"type": "customer", "id": str(fs.hero_customer_id)},
    ]
    for i in range(24):
        await _add_model(
            repo,
            tx_conn,
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[i],
            natural=(
                f"Acme SSO launch blocker evidence {i}: customer-0 cannot "
                "launch while enterprise SAML permission edge case remains open."
            ),
            scope_entities=hero_scope,
            scope_actors=[fs.hero_actor_id],
        )
    for i in range(72):
        commitment_id = fs.commitment_ids[i % len(fs.commitment_ids)]
        goal_id = fs.goal_ids[i % len(fs.goal_ids)]
        customer_id = fs.customer_resource_ids[i % len(fs.customer_resource_ids)]
        await _add_model(
            repo,
            tx_conn,
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[(i + 40) % len(fs.observation_ids)],
            natural=(
                f"Board portfolio renewal risk {i}: enterprise customer "
                f"customer-{i % 14} has runway, billing, legal, or security "
                "pressure that may affect the renewal base."
            ),
            scope_entities=[
                {"type": "commitment", "id": str(commitment_id)},
                {"type": "goal", "id": str(goal_id)},
                {"type": "customer", "id": str(customer_id)},
            ],
            scope_actors=[fs.actor_ids[i % len(fs.actor_ids)]],
        )

    total_models = await tx_conn.fetchval(
        "SELECT COUNT(*)::int FROM models WHERE tenant_id = $1 AND status = 'active'",
        tenant,
    )
    assert total_models >= 500

    cfg = InquiryConfig(
        max_rounds=1,
        questions_per_round=2,
        candidate_model_limit=180,
        result_model_limit=64,
        action_model_budget_limit=40,
        action_observation_budget_limit=20,
        persist=False,
    )
    now = datetime.now(timezone.utc)
    cases = [
        (
            "specific_blocker",
            TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=hero_scope,
                seed_natural_text=(
                    "customer-0 cannot launch because the Acme SSO SAML "
                    "permission edge case is blocked."
                ),
                seed_occurred_at=now,
                scope_actors=[fs.hero_actor_id],
                precomputed_seed_vector=make_embedding("Acme SSO launch blocker"),
            ),
        ),
        (
            "weak_noise",
            TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=hero_scope,
                seed_natural_text=(
                    "customer-0 mentioned the Thursday lunch notes and general "
                    "workspace chatter; no blocker, owner change, or decision."
                ),
                seed_occurred_at=now,
                scope_actors=[fs.hero_actor_id],
                precomputed_seed_vector=make_embedding("Thursday lunch workspace chatter"),
            ),
        ),
        (
            "broad_board_update",
            TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[],
                seed_natural_text=(
                    "Board update: across all enterprise customers, renewal "
                    "risk, runway pressure, billing disputes, legal review, and "
                    "security approvals may affect the portfolio renewal base."
                ),
                seed_occurred_at=now,
                scope_actors=[],
                precomputed_seed_vector=make_embedding("board portfolio renewal risk"),
            ),
        ),
        (
            "recurring_incident",
            TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=hero_scope[:2],
                seed_natural_text=(
                    "The Acme SSO permission incident repeated again; this may "
                    "be the same recurring launch blocker pattern."
                ),
                seed_occurred_at=now,
                scope_actors=[fs.hero_actor_id],
                precomputed_seed_vector=make_embedding("recurring SSO permission incident"),
            ),
        ),
        (
            "unrelated_chatter",
            TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[],
                seed_natural_text=(
                    "Random hallway note about office snacks and travel plans; "
                    "not related to customers, delivery, risk, or commitments."
                ),
                seed_occurred_at=now,
                scope_actors=[],
                precomputed_seed_vector=make_embedding("office snacks travel plans"),
            ),
        ),
    ]

    results = {}
    for name, trigger in cases:
        result = await run_inquiry_retrieval(
            trigger,
            tx_conn,
            mode="deep",
            top_n=180,
            config=cfg,
        )
        notes = result.retrieval_result.notes["relevance_gate"]
        results[name] = {
            "selected": len(result.retrieval_result.models),
            "candidates": notes["candidate_count"],
            "signal_class": notes["signal_class"],
            "threshold": notes["threshold"],
        }
        assert notes["candidate_count"] > 64
        assert len(result.retrieval_result.models) < notes["candidate_count"]

    assert 4 <= results["specific_blocker"]["selected"] < 32
    assert results["weak_noise"]["selected"] <= 8
    assert results["broad_board_update"]["selected"] > 32
    assert results["broad_board_update"]["selected"] <= 64
    assert 4 <= results["recurring_incident"]["selected"] < 32
    assert results["unrelated_chatter"]["selected"] <= 6
    assert results["broad_board_update"]["signal_class"] == "broad"
    assert results["weak_noise"]["signal_class"] == "weak"
