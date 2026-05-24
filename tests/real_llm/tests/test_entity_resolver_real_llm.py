"""Real-LLM checks for vague alias resolution from actual signal text."""
from __future__ import annotations

import json

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from lib.llm.provider import LLMProvider
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.workers.entity_resolver.worker import (
    EntityResolverWorker,
    ResolverLLMBudget,
)
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import Scenario, inject_sequence


@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1)
async def test_deepseek_resolves_vague_customer_alias_from_actual_content(
    scenario_04: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    provider: LLMProvider,
) -> None:
    """DeepSeek resolves `NBI` from "Nimbus Bank as NBI" in signal text."""
    source_signal = next(
        signal
        for signal in scenario_04.get_sequence(
            "retrieval_alias_drift_and_duplicate_customers"
        )
        if "Nimbus Bank as NBI" in signal["content"]
    )
    sequence_name = "nbi_alias_resolution_probe"
    scenario_04.signal_sequences[sequence_name] = [source_signal]

    obs_ids = await inject_sequence(
        scenario_04,
        sequence_name,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
        run_id="deepseek-entity-resolver-alias-test",
    )
    obs_id = obs_ids[0]
    expected_ref = {
        "type": "customer",
        "id": str(scenario_04.customer_id("Nimbus Bank")),
    }

    async with fresh_db.acquire() as conn:
        # Keep this real-LLM check to one resolver call. Ingestion has already
        # scanned actual content; this narrows the unresolved queue to the
        # alias whose behavior we are proving.
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
            scenario_04.tenant_id,
            obs_id,
            json.dumps(["NBI"]),
        )

    worker = EntityResolverWorker(
        pool=fresh_db,
        llm=provider,
        alias_repo=alias_repo,
        budget=ResolverLLMBudget(per_minute=1000),
    )
    decisions = await worker.process_observation(obs_id, scenario_04.tenant_id)

    assert decisions == [("NBI", "resolved")]
    ref = await alias_repo.fast_path_resolve("NBI", scenario_04.tenant_id)
    assert ref == expected_ref

    async with fresh_db.acquire() as conn:
        entities = await conn.fetchval(
            """
            SELECT entities_mentioned
            FROM observations
            WHERE tenant_id = $1 AND id = $2
            """,
            scenario_04.tenant_id,
            obs_id,
        )
    assert expected_ref in list(entities or [])
