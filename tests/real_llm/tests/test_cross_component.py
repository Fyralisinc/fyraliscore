"""Real-LLM end-to-end cross-component flows: ingestion -> Think -> Models -> cascade."""
from __future__ import annotations

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from tests.real_llm.infrastructure.assertion_helpers import (
    assert_at_least_one_model_matching,
    assert_cascade_chain_intact,
)
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import (
    Scenario,
    inject_sequence,
)
from tests.real_llm.infrastructure.think_drain import (
    load_active_models,
    wait_for_think_to_drain,
)


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=900)
async def test_full_flow_github_merge_to_models_to_state_change(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    sequence = scenario_02.get_sequence("alice_ships_refund_flow")
    expected_obs_count = len(sequence)
    assert expected_obs_count == 8, (
        f"alice_ships_refund_flow should be 8 signals (scenario invariant); "
        f"got {expected_obs_count}"
    )

    obs_ids = await inject_sequence(
        scenario_02,
        "alice_ships_refund_flow",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    assert len(obs_ids) == expected_obs_count

    await wait_for_think_to_drain(
        scenario_02.tenant_id,
        fresh_db,
        timeout_seconds=300,
    )

    # (a) All 8 injected signals are now Observation rows for the tenant.
    async with fresh_db.acquire() as conn:
        signal_obs_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            """,
            scenario_02.tenant_id,
            obs_ids,
        )
    assert int(signal_obs_count or 0) == expected_obs_count, (
        f"expected all {expected_obs_count} injected signals to land as "
        f"Observation rows for tenant {scenario_02.tenant_id}; got "
        f"{signal_obs_count}"
    )

    # (b) At least 2 Models exist scoped to Alice OR to the refund-flow commitment.
    alice_id = scenario_02.actor_id("Alice Chen")
    refund_commitment_id = scenario_02.commitment_id("Implement refund flow")

    alice_models = await load_active_models(
        scenario_02.tenant_id, fresh_db, scope_actor_id=alice_id
    )
    refund_models = await load_active_models(
        scenario_02.tenant_id,
        fresh_db,
        scope_entity_id=refund_commitment_id,
    )
    relevant_model_ids = {m.id for m in alice_models} | {m.id for m in refund_models}
    assert len(relevant_model_ids) >= 2, (
        f"expected >=2 Models scoped to Alice ({alice_id}) OR refund-flow "
        f"commitment ({refund_commitment_id}); got {len(relevant_model_ids)} "
        f"(alice={len(alice_models)}, refund={len(refund_models)})"
    )

    # (c) At least 1 state_change observation exists in the cascade chain
    # (cause_id traceable back to one of the original signal observations).
    async with fresh_db.acquire() as conn:
        state_change_count = await conn.fetchval(
            """
            WITH RECURSIVE chain AS (
              SELECT id, cause_id, kind
              FROM observations
              WHERE tenant_id = $1
                AND id = ANY($2::uuid[])
              UNION
              SELECT o.id, o.cause_id, o.kind
              FROM observations o
              JOIN chain c ON o.cause_id = c.id
              WHERE o.tenant_id = $1
            )
            SELECT COUNT(*)::bigint
            FROM chain
            WHERE kind = 'state_change'
            """,
            scenario_02.tenant_id,
            obs_ids,
        )
    assert int(state_change_count or 0) >= 1, (
        f"expected >=1 state_change observation in cascade chain rooted at the "
        f"{expected_obs_count} injected signals; got {state_change_count}"
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=900)
async def test_customer_crisis_to_intervention_signal_chain(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    obs_ids = await inject_sequence(
        scenario_02,
        "customer_churn_signal",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    assert len(obs_ids) >= 1
    customer_email_obs_id = obs_ids[0]  # external Globex contact email

    await wait_for_think_to_drain(
        scenario_02.tenant_id,
        fresh_db,
        timeout_seconds=300,
    )

    globex_id = scenario_02.customer_id("Globex Inc")

    # (a) Globex's at-risk Model(s) exist with risk-language proposition.
    globex_models = await load_active_models(
        scenario_02.tenant_id, fresh_db, scope_entity_id=globex_id
    )
    assert_at_least_one_model_matching(
        globex_models,
        scope_entity_id=globex_id,
        proposition_text_contains=[
            "risk",
            "churn",
            "concern",
            "evaluating",
            "renewal",
        ],
        context=(
            "Globex churn email + sales triage should produce at least one "
            "Model scoped to Globex with risk/renewal language"
        ),
    )

    # (b) At least one Model is scoped to a sales/CS team member.
    # Carmen Diaz owns customer relations / sales for the Globex thread per
    # scenario_02 (she triages the email and books the call); Henry Sato is
    # customer success and joins. Either qualifies.
    carmen_id = scenario_02.actor_id("Carmen Diaz")
    henry_id = scenario_02.actor_id("Henry Sato")
    carmen_models = await load_active_models(
        scenario_02.tenant_id, fresh_db, scope_actor_id=carmen_id
    )
    henry_models = await load_active_models(
        scenario_02.tenant_id, fresh_db, scope_actor_id=henry_id
    )
    sales_cs_model_count = len({m.id for m in carmen_models} | {m.id for m in henry_models})
    assert sales_cs_model_count >= 1, (
        f"expected >=1 Model scoped to Carmen Diaz ({carmen_id}) or "
        f"Henry Sato ({henry_id}); got {sales_cs_model_count} "
        f"(carmen={len(carmen_models)}, henry={len(henry_models)})"
    )

    # (c) Cause_id chain from the customer's email observation walks >=2 hops.
    await assert_cascade_chain_intact(
        scenario_02.tenant_id,
        customer_email_obs_id,
        pool=fresh_db,
        min_depth=2,
        context=(
            "Customer churn email should produce at least a 2-hop cascade "
            "chain (think run -> downstream observations)"
        ),
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=900)
async def test_feature_launch_produces_coordinated_models_across_actors(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_02,
        "feature_launch_cycle",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )

    await wait_for_think_to_drain(
        scenario_02.tenant_id,
        fresh_db,
        timeout_seconds=300,
    )

    models = await load_active_models(scenario_02.tenant_id, fresh_db)

    # (b) Total Model count >=3 (Think recognized this as multi-actor work).
    assert len(models) >= 3, (
        f"expected >=3 Models from feature_launch_cycle (multi-actor "
        f"coordination); got {len(models)}"
    )

    # (a) At least 3 distinct actors appear across scope_actors of all Models.
    distinct_scope_actors: set = set()
    for m in models:
        for actor_uuid in m.scope_actors:
            distinct_scope_actors.add(actor_uuid)
    assert len(distinct_scope_actors) >= 3, (
        f"expected >=3 distinct actors across scope_actors of "
        f"{len(models)} Models; got {len(distinct_scope_actors)} "
        f"({sorted(str(a) for a in distinct_scope_actors)})"
    )


@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=900)
async def test_early_startup_founder_disagreement_produces_contestation_signals(
    scenario_01: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    await inject_sequence(
        scenario_01,
        "founder_debate",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )

    await wait_for_think_to_drain(
        scenario_01.tenant_id,
        fresh_db,
        timeout_seconds=300,
    )

    models = await load_active_models(scenario_01.tenant_id, fresh_db)
    assert models, (
        "founder_debate sequence should produce at least one Model"
    )

    # (a) At least one Model has non-empty signal_readings.
    has_signal_readings = any(
        bool(m.signal_readings) for m in models
    )
    if has_signal_readings:
        return

    # Fallback: at least 2 Models reference the same scope_entity. This is
    # less LLM-dependent than text-pattern matching for opposing positions.
    scope_entity_keys: list[tuple[str | None, str | None]] = []
    for m in models:
        for entry in m.scope_entities:
            scope_entity_keys.append(
                (entry.get("type"), str(entry.get("id")) if entry.get("id") is not None else None)
            )

    overlap_count = 0
    seen_keys: set[tuple[str | None, str | None]] = set()
    duplicate_keys: set[tuple[str | None, str | None]] = set()
    for key in scope_entity_keys:
        if key in seen_keys:
            duplicate_keys.add(key)
        seen_keys.add(key)
    overlap_count = len(duplicate_keys)

    assert overlap_count >= 1, (
        f"founder_debate produced {len(models)} Models with no "
        f"signal_readings populated AND no overlapping scope_entity across "
        f"Models; expected either contestation-signal evidence OR at least "
        f"two Models referencing the same scope_entity. "
        f"scope_entity_keys={scope_entity_keys}"
    )
