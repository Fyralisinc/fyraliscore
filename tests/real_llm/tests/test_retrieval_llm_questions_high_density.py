"""Real-LLM high-density retrieval question planning checks."""
from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
import pytest

from lib.llm.provider import LLMProvider
from lib.shared.types import ModelCreate
from services.platform.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.domain.models.repo import ModelsRepo, pgvector_pool_init
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test


async def _add_model(
    repo: ModelsRepo,
    conn: asyncpg.Connection,
    *,
    tenant_id,
    born_from_event_id,
    natural: str,
    scope_entities: list[dict],
    scope_actors: list,
) -> None:
    await repo.insert(
        ModelCreate(
            tenant_id=tenant_id,
            born_from_event_id=born_from_event_id,
            proposition={
                "kind": "concern",
                "about": natural[:80],
                "nature": "risk",
                "raised_by": "real-llm-high-density-retrieval-test",
            },
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


@pytest.mark.asyncio
@real_llm_test(
    attempts=1,
    pass_threshold=1,
    timeout_seconds=900,
    tags=["retrieval", "llm-question-planning", "high-density"],
)
async def test_real_llm_question_planning_on_high_density_retrieval(
    fresh_db: asyncpg.Pool,
    tenant_id,
    provider: LLMProvider,
) -> None:
    conn = await fresh_db.acquire()
    tx = conn.transaction()
    await tx.start()
    try:
        await pgvector_pool_init(conn)
        await conn.execute("SET CONSTRAINTS ALL DEFERRED")
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, 'real_llm_high_density_retrieval', true)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
        )
        fs = await build_fixture(
            conn,
            tenant_id,
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
                conn,
                tenant_id=tenant_id,
                born_from_event_id=fs.observation_ids[i],
                natural=(
                    f"Acme SSO launch blocker evidence {i}: customer-0 cannot "
                    "launch while enterprise SAML permission edge case remains open."
                ),
                scope_entities=hero_scope,
                scope_actors=[fs.hero_actor_id],
            )
        for i in range(72):
            await _add_model(
                repo,
                conn,
                tenant_id=tenant_id,
                born_from_event_id=fs.observation_ids[(i + 40) % len(fs.observation_ids)],
                natural=(
                    f"Board portfolio renewal risk {i}: enterprise customer "
                    f"customer-{i % 14} has runway, billing, legal, or security "
                    "pressure that may affect the renewal base."
                ),
                scope_entities=[
                    {
                        "type": "commitment",
                        "id": str(fs.commitment_ids[i % len(fs.commitment_ids)]),
                    },
                    {"type": "goal", "id": str(fs.goal_ids[i % len(fs.goal_ids)])},
                    {
                        "type": "customer",
                        "id": str(
                            fs.customer_resource_ids[
                                i % len(fs.customer_resource_ids)
                            ]
                        ),
                    },
                ],
                scope_actors=[fs.actor_ids[i % len(fs.actor_ids)]],
            )

        total_models = await conn.fetchval(
            "SELECT COUNT(*)::int FROM models WHERE tenant_id = $1 AND status = 'active'",
            tenant_id,
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
        cases = {
            "specific_blocker": TriggerContext(
                kind="T1",
                tenant_id=tenant_id,
                seed_entity_ids=hero_scope,
                seed_natural_text=(
                    "customer-0 cannot launch because the Acme SSO SAML "
                    "permission edge case is blocked."
                ),
                seed_occurred_at=now,
                scope_actors=[fs.hero_actor_id],
                precomputed_seed_vector=make_embedding("Acme SSO launch blocker"),
            ),
            "weak_noise": TriggerContext(
                kind="T1",
                tenant_id=tenant_id,
                seed_entity_ids=hero_scope,
                seed_natural_text=(
                    "customer-0 mentioned the Thursday lunch notes and general "
                    "workspace chatter; no blocker, owner change, or decision."
                ),
                seed_occurred_at=now,
                scope_actors=[fs.hero_actor_id],
                precomputed_seed_vector=make_embedding("Thursday lunch workspace chatter"),
            ),
            "broad_board_update": TriggerContext(
                kind="T1",
                tenant_id=tenant_id,
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
            "recurring_incident": TriggerContext(
                kind="T1",
                tenant_id=tenant_id,
                seed_entity_ids=hero_scope[:2],
                seed_natural_text=(
                    "The Acme SSO permission incident repeated again; this may "
                    "be the same recurring launch blocker pattern."
                ),
                seed_occurred_at=now,
                scope_actors=[fs.hero_actor_id],
                precomputed_seed_vector=make_embedding(
                    "recurring SSO permission incident"
                ),
            ),
            "unrelated_chatter": TriggerContext(
                kind="T1",
                tenant_id=tenant_id,
                seed_entity_ids=[],
                seed_natural_text=(
                    "Random hallway note about office snacks and travel plans; "
                    "not related to customers, delivery, risk, or commitments."
                ),
                seed_occurred_at=now,
                scope_actors=[],
                precomputed_seed_vector=make_embedding("office snacks travel plans"),
            ),
        }

        results = {}
        for name, trigger in cases.items():
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                llm_provider=provider,
                mode="deep",
                top_n=180,
                config=cfg,
            )
            relevance = result.retrieval_result.notes["relevance_gate"]
            planning = result.notes["question_planning"][0]
            results[name] = {
                "total_models": total_models,
                "selected": len(result.retrieval_result.models),
                "candidates": relevance["candidate_count"],
                "signal_class": relevance["signal_class"],
                "question_mode": planning["mode"],
                "llm_primitives": planning.get("llm_primitives", []),
            }
            assert planning["mode"] == "llm"
            assert relevance["candidate_count"] > 64
            assert len(result.retrieval_result.models) < relevance["candidate_count"]

        print("REAL_LLM_HIGH_DENSITY_RETRIEVAL", results)
        assert 4 <= results["specific_blocker"]["selected"] < 32
        assert results["weak_noise"]["selected"] <= 10
        assert 24 <= results["broad_board_update"]["selected"] <= 64
        assert 3 <= results["recurring_incident"]["selected"] < 40
        assert results["unrelated_chatter"]["selected"] <= 8
        assert results["broad_board_update"]["signal_class"] == "broad"
        assert results["weak_noise"]["signal_class"] == "weak"
    finally:
        await tx.rollback()
        await fresh_db.release(conn)
