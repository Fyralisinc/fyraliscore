"""Opt-in full-corpus ingestion check for the scale-chaos scenario.

This does not call a real LLM or wait for Think. It materializes scenario 04
and pushes every synthetic signal through the same ingestion path a customer
webhook would use: actor resolution, alias fast-path, embeddings, observation
insert, and T1 trigger enqueue.

It is opt-in because it embeds 100+ signals and is meant for stress validation,
not every local real-LLM loop.
"""
from __future__ import annotations

import os

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import Scenario, inject_sequence


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("RUN_SCALE_CHAOS_FULL") != "1",
    reason="set RUN_SCALE_CHAOS_FULL=1 to inject all scenario_04 signals",
)
@real_llm_test(attempts=1, pass_threshold=1)
async def test_scenario_04_injects_full_scale_chaos_corpus(
    scenario_04: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    expected_signal_count = sum(
        len(sequence) for sequence in scenario_04.signal_sequences.values()
    )

    observation_ids = []
    for sequence_name in scenario_04.signal_sequences:
        observation_ids.extend(
            await inject_sequence(
                scenario_04,
                sequence_name,
                pool=fresh_db,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                time_compression=0.0,
                run_id="scale-chaos-full-ingestion-test",
            )
        )

    assert len(observation_ids) == expected_signal_count
    assert len(set(observation_ids)) == expected_signal_count

    async with fresh_db.acquire() as conn:
        synthetic_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'scenario_id' = 'scale_chaos_b2b'
              AND content->>'run_id' = 'scale-chaos-full-ingestion-test'
            """,
            scenario_04.tenant_id,
        )
        trigger_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
            """,
            scenario_04.tenant_id,
        )
        distinct_channels = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT source_channel)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'scenario_id' = 'scale_chaos_b2b'
            """,
            scenario_04.tenant_id,
        )
        with_entities = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM observations
            WHERE tenant_id = $1
              AND content->>'scenario_id' = 'scale_chaos_b2b'
              AND jsonb_array_length(entities_mentioned) > 0
            """,
            scenario_04.tenant_id,
        )

    assert int(synthetic_count or 0) == expected_signal_count
    assert int(trigger_count or 0) >= expected_signal_count
    assert int(distinct_channels or 0) >= 9
    assert int(with_entities or 0) >= expected_signal_count // 2
