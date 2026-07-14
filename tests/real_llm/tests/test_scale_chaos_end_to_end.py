"""Scenario 04 end-to-end checks across ingestion, alias resolution, Think, and Bridge."""
from __future__ import annotations

import asyncio
import json
import os
from decimal import Decimal

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from lib.llm.provider import LLMProvider
from services.domain.actors.repo import ActorRepo
from services.domain.bridge.dashboards import CustomerDetailDashboard, render_customer_detail
from services.domain.bridge.queries import RevenueAtRiskReport, revenue_at_risk
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.reasoning.think.worker import ThinkWorker, WorkerConfig
from services.workers.entity_resolver.worker import (
    EntityResolverWorker,
    ResolverLLMBudget,
)
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import Scenario, inject_sequence
from tests.real_llm.infrastructure.think_drain import (
    load_active_models,
    wait_for_think_to_drain,
)


@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1, timeout_seconds=900)
async def test_scenario_04_nimbus_alias_to_think_to_bridge_end_to_end(
    scenario_04: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    provider: LLMProvider,
) -> None:
    """Full proof path: signal text alias -> DeepSeek resolver -> Think -> Bridge."""
    assert scenario_04.tenant_id is not None
    tenant_id = scenario_04.tenant_id

    alias_obs_id = await _inject_nbi_alias_signal(
        scenario_04,
        fresh_db=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
    )
    expected_nimbus_ref = {
        "type": "customer",
        "id": str(scenario_04.customer_id("Nimbus Bank")),
    }

    await _resolve_single_alias_phrase(
        alias_obs_id,
        "NBI",
        tenant_id=tenant_id,
        pool=fresh_db,
        alias_repo=alias_repo,
        provider=provider,
    )
    assert await alias_repo.fast_path_resolve("NBI", tenant_id) == expected_nimbus_ref

    full_nimbus_sequence = scenario_04.get_sequence("nimbus_audit_and_saml_pressure")
    curated_sequence_name = "nimbus_end_to_end_curated"
    scenario_04.signal_sequences[curated_sequence_name] = full_nimbus_sequence[:6]

    nimbus_obs_ids = await inject_sequence(
        scenario_04,
        curated_sequence_name,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
        run_id="scenario-04-end-to-end-nimbus",
    )
    assert len(nimbus_obs_ids) == 6

    await _run_think_until_drain(
        tenant_id,
        pool=fresh_db,
        provider=provider,
        timeout_seconds=600,
    )

    await _assert_think_processed_end_to_end(
        tenant_id,
        pool=fresh_db,
        original_obs_ids=[alias_obs_id, *nimbus_obs_ids],
    )
    await _assert_nimbus_memory_created(scenario_04, pool=fresh_db)
    await _assert_nimbus_bridge_surfaces_risk(scenario_04, pool=fresh_db)


async def _inject_nbi_alias_signal(
    scenario: Scenario,
    *,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
):
    source_signal = next(
        signal
        for signal in scenario.get_sequence(
            "retrieval_alias_drift_and_duplicate_customers"
        )
        if "Nimbus Bank as NBI" in signal["content"]
    )
    sequence_name = "nbi_alias_end_to_end_probe"
    scenario.signal_sequences[sequence_name] = [source_signal]
    obs_ids = await inject_sequence(
        scenario,
        sequence_name,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
        run_id="scenario-04-end-to-end-alias",
    )
    return obs_ids[0]


async def _resolve_single_alias_phrase(
    observation_id,
    phrase: str,
    *,
    tenant_id,
    pool: asyncpg.Pool,
    alias_repo: EntityAliasRepo,
    provider: LLMProvider,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE observations
            SET content = jsonb_set(
                content,
                '{_unresolved_phrases}',
                $3::jsonb,
                true
            )
            WHERE tenant_id = $1 AND id = $2
            """,
            tenant_id,
            observation_id,
            json.dumps([phrase]),
        )

    worker = EntityResolverWorker(
        pool=pool,
        llm=provider,
        alias_repo=alias_repo,
        budget=ResolverLLMBudget(per_minute=1000),
    )
    decisions = await worker.process_observation(observation_id, tenant_id)
    assert decisions == [(phrase, "resolved")]


async def _run_think_until_drain(
    tenant_id,
    *,
    pool: asyncpg.Pool,
    provider: LLMProvider,
    timeout_seconds: int,
) -> None:
    cfg = WorkerConfig.from_env()
    cfg.poll_interval_s = 0.05
    cfg.max_concurrency_per_tenant = int(
        os.environ.get(
            "REAL_LLM_THINK_CONCURRENCY",
            cfg.max_concurrency_per_tenant,
        )
    )
    cfg.trigger_max_attempts = int(
        os.environ.get(
            "REAL_LLM_TRIGGER_MAX_ATTEMPTS",
            cfg.trigger_max_attempts,
        )
    )
    cfg.tenant_filter = tenant_id
    cfg.t1_batch_window_s = float(
        os.environ.get("REAL_LLM_T1_BATCH_WINDOW_S", 0.0)
    )
    cfg.downstream_batch_window_s = float(
        os.environ.get("REAL_LLM_DOWNSTREAM_BATCH_WINDOW_S", 0.0)
    )
    cfg.process_background_triggers = (
        os.environ.get("REAL_LLM_PROCESS_BACKGROUND_TRIGGERS", "0")
        .strip()
        .lower()
        not in {"0", "false", "no", "off"}
    )
    worker = ThinkWorker(pool=pool, config=cfg, llm_provider=provider)

    async def _noop_promote() -> None:
        return None

    if not cfg.process_background_triggers:
        worker._promote_reeval_rows = _noop_promote  # type: ignore[assignment]
    task = asyncio.create_task(worker.run())
    from services.reasoning.think.post_commit import post_commit_worker

    post_commit_stop = asyncio.Event()
    post_commit_task = asyncio.create_task(
        post_commit_worker(
            pool,
            poll_interval=float(
                os.environ.get("REAL_LLM_POST_COMMIT_POLL_INTERVAL_S", 0.05)
            ),
            stop_event=post_commit_stop,
            tenant_id=tenant_id,
        )
    )
    try:
        await wait_for_think_to_drain(
            tenant_id,
            pool,
            timeout_seconds=timeout_seconds,
            poll_interval_s=0.5,
        )
    finally:
        post_commit_stop.set()
        try:
            await asyncio.wait_for(post_commit_task, timeout=10)
        except asyncio.TimeoutError:
            post_commit_task.cancel()
            try:
                await post_commit_task
            except (asyncio.CancelledError, Exception):
                pass
        await worker.stop()
        try:
            await asyncio.wait_for(task, timeout=10)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


async def _assert_think_processed_end_to_end(
    tenant_id,
    *,
    pool: asyncpg.Pool,
    original_obs_ids: list,
) -> None:
    async with pool.acquire() as conn:
        successful_runs = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_runs
            WHERE tenant_id = $1
              AND status IN ('success', 'succeeded')
            """,
            tenant_id,
        )
        downstream_state_changes = await conn.fetchval(
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
            tenant_id,
            original_obs_ids,
        )

    assert int(successful_runs or 0) >= 1
    assert int(downstream_state_changes or 0) >= 1


async def _assert_nimbus_memory_created(
    scenario: Scenario,
    *,
    pool: asyncpg.Pool,
) -> None:
    tenant_id = scenario.tenant_id
    assert tenant_id is not None
    nimbus_id = scenario.customer_id("Nimbus Bank")
    audit_commitment_id = scenario.commitment_id("Finish Nimbus audit export rollout")
    saml_commitment_id = scenario.commitment_id("Stabilize Nimbus SAML incident response")

    all_models = await load_active_models(tenant_id, pool)
    scoped_models = []
    for entity_id in (nimbus_id, audit_commitment_id, saml_commitment_id):
        scoped_models.extend(
            await load_active_models(
                tenant_id,
                pool,
                scope_entity_id=entity_id,
            )
        )
    relevant_model_ids = {m.id for m in scoped_models}
    relevant_by_text = [
        m
        for m in all_models
        if any(
            term in m.natural.lower()
            for term in ("nimbus", "saml", "audit", "renewal")
        )
    ]

    assert relevant_model_ids or relevant_by_text, (
        "Expected Think to create at least one Nimbus/SAML/audit/renewal "
        f"Model; saw {len(all_models)} active Models"
    )


async def _assert_nimbus_bridge_surfaces_risk(
    scenario: Scenario,
    *,
    pool: asyncpg.Pool,
) -> None:
    tenant_id = scenario.tenant_id
    assert tenant_id is not None
    nimbus_id = scenario.customer_id("Nimbus Bank")

    async with pool.acquire() as conn:
        report: RevenueAtRiskReport = await revenue_at_risk(tenant_id, conn=conn)
        detail: CustomerDetailDashboard = await render_customer_detail(
            nimbus_id,
            tenant_id=tenant_id,
            window_days=30,
            conn=conn,
        )

    nimbus_row = next(
        (row for row in report.customers if row.customer_resource_id == nimbus_id),
        None,
    )
    assert nimbus_row is not None, (
        "Nimbus Bank missing from revenue-at-risk report after end-to-end run"
    )
    assert nimbus_row.total_at_risk_usd >= Decimal("0")

    assert detail.customer_resource_id == nimbus_id
    assert detail.identity == "Nimbus Bank"
    assert detail.arr_usd == Decimal("2400000")
    served_titles = {item.title for item in detail.served_commitments}
    assert "Finish Nimbus audit export rollout" in served_titles
    assert "Stabilize Nimbus SAML incident response" in served_titles
