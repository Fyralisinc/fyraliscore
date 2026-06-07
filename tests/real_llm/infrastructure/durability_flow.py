"""Shared full-signal durability helpers for real-LLM scenarios."""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from lib.embeddings.ollama import OllamaClient
from lib.llm.provider import LLMProvider
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.think.worker import ThinkWorker, WorkerConfig
from services.workers.entity_resolver.worker import (
    EntityResolverWorker,
    ResolverLLMBudget,
)
from tests.real_llm.infrastructure.scenario_loader import Scenario, inject_sequence
from tests.real_llm.infrastructure.think_drain import (
    load_active_models,
    wait_for_think_to_drain,
)


@dataclass(frozen=True)
class FullSignalSummary:
    """Observable health summary after a durability run drains Think."""

    scenario_id: str
    run_id: str
    expected_signal_count: int
    observation_count: int
    unique_observation_count: int
    trigger_count: int
    pending_triggers: int
    successful_runs: int
    failed_runs: int
    skipped_runs: int
    context_use_reports: int
    downstream_state_changes: int
    active_models: int
    distinct_channels: int
    observations_with_entities: int


async def inject_all_sequences(
    scenario: Scenario,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    run_id: str,
) -> dict[str, list[UUID]]:
    """Inject every signal in every sequence, preserving sequence order."""
    observation_ids_by_sequence: dict[str, list[UUID]] = {}
    for sequence_name in scenario.signal_sequences:
        observation_ids_by_sequence[sequence_name] = await inject_sequence(
            scenario,
            sequence_name,
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            time_compression=0.0,
            run_id=run_id,
        )
    return observation_ids_by_sequence


def flatten_observation_ids(
    observation_ids_by_sequence: dict[str, list[UUID]],
) -> list[UUID]:
    """Return injected observation IDs in corpus order."""
    return [
        observation_id
        for sequence_ids in observation_ids_by_sequence.values()
        for observation_id in sequence_ids
    ]


def observation_id_for_signal_text(
    scenario: Scenario,
    observation_ids_by_sequence: dict[str, list[UUID]],
    *,
    sequence_name: str,
    text_needle: str,
) -> UUID:
    """Find the injected observation that corresponds to a YAML signal text."""
    sequence = scenario.get_sequence(sequence_name)
    for index, signal in enumerate(sequence):
        content = str(signal.get("content") or signal.get("text") or "")
        if text_needle in content:
            return observation_ids_by_sequence[sequence_name][index]
    raise AssertionError(
        f"Could not find signal containing {text_needle!r} in {sequence_name!r}"
    )


async def resolve_alias_phrase_from_observation(
    observation_id: UUID,
    phrase: str,
    *,
    tenant_id: UUID,
    pool: asyncpg.Pool,
    alias_repo: EntityAliasRepo,
    provider: LLMProvider,
) -> None:
    """Force the actual-content alias resolver to inspect `phrase`."""
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


async def run_think_until_drain(
    tenant_id: UUID,
    *,
    pool: asyncpg.Pool,
    provider: LLMProvider,
    timeout_seconds: int,
) -> None:
    """Run the production Think worker until this tenant's queue is drained."""
    cfg = WorkerConfig.from_env()
    cfg.poll_interval_s = 0.05
    cfg.tenant_filter = tenant_id
    cfg.max_concurrency_per_tenant = int(
        os.environ.get(
            "DURABILITY_THINK_CONCURRENCY",
            cfg.max_concurrency_per_tenant,
        )
    )
    cfg.trigger_max_attempts = int(
        os.environ.get(
            "DURABILITY_TRIGGER_MAX_ATTEMPTS",
            cfg.trigger_max_attempts,
        )
    )
    worker = ThinkWorker(pool=pool, config=cfg, llm_provider=provider)

    async def _noop_promote() -> None:
        return None

    # Keep the durability run focused on T1 signal processing. T4 fanout is
    # covered elsewhere and can turn a corpus test into an unbounded cascade.
    worker._promote_reeval_rows = _noop_promote  # type: ignore[assignment]
    task = asyncio.create_task(worker.run())
    try:
        await wait_for_think_to_drain(
            tenant_id,
            pool,
            timeout_seconds=timeout_seconds,
            poll_interval_s=0.5,
        )
    finally:
        await worker.stop()
        try:
            await asyncio.wait_for(task, timeout=10)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


async def collect_full_signal_summary(
    scenario: Scenario,
    *,
    run_id: str,
    pool: asyncpg.Pool,
    original_observation_ids: list[UUID],
) -> FullSignalSummary:
    """Collect DB-level evidence that the full-signal flow completed."""
    assert scenario.tenant_id is not None
    tenant_id = scenario.tenant_id
    expected_signal_count = sum(
        len(sequence) for sequence in scenario.signal_sequences.values()
    )
    active_models = await load_active_models(tenant_id, pool)

    async with pool.acquire() as conn:
        observation_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'scenario_id' = $2
              AND content->>'run_id' = $3
            """,
            tenant_id,
            scenario.scenario_id,
            run_id,
        )
        trigger_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        pending_triggers = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
            """,
            tenant_id,
        )
        successful_runs = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_runs
            WHERE tenant_id = $1
              AND status = 'success'
            """,
            tenant_id,
        )
        failed_runs = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_runs
            WHERE tenant_id = $1
              AND status = 'failed'
            """,
            tenant_id,
        )
        skipped_runs = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_runs
            WHERE tenant_id = $1
              AND status = 'skipped_idempotent'
            """,
            tenant_id,
        )
        context_use_reports = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_runs
            WHERE tenant_id = $1
              AND status = 'success'
              AND ops_applied ? 'context_use'
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
            original_observation_ids,
        )
        distinct_channels = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT source_channel)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'scenario_id' = $2
              AND content->>'run_id' = $3
            """,
            tenant_id,
            scenario.scenario_id,
            run_id,
        )
        observations_with_entities = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'scenario_id' = $2
              AND content->>'run_id' = $3
              AND jsonb_typeof(entities_mentioned) = 'array'
              AND jsonb_array_length(entities_mentioned) > 0
            """,
            tenant_id,
            scenario.scenario_id,
            run_id,
        )

    return FullSignalSummary(
        scenario_id=scenario.scenario_id,
        run_id=run_id,
        expected_signal_count=expected_signal_count,
        observation_count=int(observation_count or 0),
        unique_observation_count=len(set(original_observation_ids)),
        trigger_count=int(trigger_count or 0),
        pending_triggers=int(pending_triggers or 0),
        successful_runs=int(successful_runs or 0),
        failed_runs=int(failed_runs or 0),
        skipped_runs=int(skipped_runs or 0),
        context_use_reports=int(context_use_reports or 0),
        downstream_state_changes=int(downstream_state_changes or 0),
        active_models=len(active_models),
        distinct_channels=int(distinct_channels or 0),
        observations_with_entities=int(observations_with_entities or 0),
    )


async def assert_customer_commitment_links(
    scenario: Scenario,
    *,
    pool: asyncpg.Pool,
    customer_name: str,
    required_commitments: set[str],
) -> None:
    """Verify customer memory remains linked after the stress run."""
    assert scenario.tenant_id is not None
    customer_id = scenario.customer_id(customer_name)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.title
            FROM customer_commitments cc
            JOIN commitments c ON c.id = cc.commitment_id
            WHERE cc.tenant_id = $1
              AND cc.customer_resource_id = $2
            """,
            scenario.tenant_id,
            customer_id,
        )
    served_titles = {str(row["title"]) for row in rows}
    missing = required_commitments - served_titles
    assert not missing, f"{customer_name} missing commitment links: {missing}"
