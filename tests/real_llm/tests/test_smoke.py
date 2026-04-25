"""Phase 2 smoke tests for the real-LLM test suite.

Sanity checks that scenario materialization, signal injection, and the
T1 Think-trigger enqueue work end-to-end against real Postgres + real
Ollama embeddings. These tests deliberately:

* Inject only the FIRST signal of each scenario's first sequence — never
  the full sequence — so wall time stays trivial.
* Do NOT await Think completion (`wait_for_think_to_drain` is not
  called), so no DeepSeek tokens are spent. The assertion on the
  trigger queue is purely structural: the row was enqueued.

If you find yourself adding `wait_for_think_to_drain(...)` to a smoke
test, stop — that belongs in Phase 3 (the behavioural tests), not here.
"""
from __future__ import annotations

import asyncpg
import pytest

from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from lib.embeddings.ollama import OllamaClient
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import (
    Scenario,
    inject_sequence,
)


async def _smoke_check(
    scenario: Scenario,
    sequence_name: str,
    *,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    """Shared body: assert materialize populated IDs, inject 1 signal, verify DB rows."""
    # --- assert materialize populated the resolved-IDs maps ---
    assert scenario.tenant_id is not None, "scenario.tenant_id should be set after materialize"
    assert len(scenario.actors) >= 1, (
        f"expected >=1 actor for {scenario.scenario_id}, got {len(scenario.actors)}"
    )
    assert len(scenario.customers) >= 1, (
        f"expected >=1 customer for {scenario.scenario_id}, got {len(scenario.customers)}"
    )
    assert len(scenario.goals) >= 1, (
        f"expected >=1 goal for {scenario.scenario_id}, got {len(scenario.goals)}"
    )
    assert len(scenario.commitments) >= 1, (
        f"expected >=1 commitment for {scenario.scenario_id}, "
        f"got {len(scenario.commitments)}"
    )

    # --- shrink the sequence to its first signal so injection is cheap ---
    full_sequence = scenario.get_sequence(sequence_name)
    assert len(full_sequence) >= 1, (
        f"sequence {sequence_name!r} is empty in {scenario.scenario_id}"
    )
    scenario.signal_sequences[sequence_name] = full_sequence[:1]

    obs_ids = await inject_sequence(
        scenario,
        sequence_name,
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )

    assert len(obs_ids) == 1, (
        f"expected exactly 1 observation id back, got {len(obs_ids)}"
    )
    assert obs_ids[0] is not None, "inject_sequence returned a None observation id"

    # --- DB-level verification: the observation row exists for this tenant ---
    async with fresh_db.acquire() as conn:
        obs_row_id = await conn.fetchval(
            """
            SELECT id
            FROM observations
            WHERE tenant_id = $1
              AND id = $2
            """,
            scenario.tenant_id,
            obs_ids[0],
        )
        assert obs_row_id == obs_ids[0], (
            f"observation row not found for tenant {scenario.tenant_id} "
            f"id {obs_ids[0]}"
        )

        # --- Think trigger was enqueued for this tenant ---
        # The bootstrap observation is INSERTed directly (no T1), so
        # any rows here come from the synthetic signal we just injected.
        trigger_count = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
            """,
            scenario.tenant_id,
        )
        assert int(trigger_count or 0) >= 1, (
            f"expected >=1 think_trigger_queue row for tenant "
            f"{scenario.tenant_id}, got {trigger_count}"
        )


@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1)
async def test_scenario_01_materializes_and_injects_first_signal(
    scenario_01: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    """Scenario 01 (early_startup): materialize + first signal of founder_debate."""
    await _smoke_check(
        scenario_01,
        "founder_debate",
        fresh_db=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
    )


@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1)
async def test_scenario_02_materializes_and_injects_first_signal(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    """Scenario 02 (growth_saas): materialize + first signal of alice_ships_refund_flow."""
    await _smoke_check(
        scenario_02,
        "alice_ships_refund_flow",
        fresh_db=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
    )


@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1)
async def test_scenario_03_materializes_and_injects_first_signal(
    scenario_03: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
) -> None:
    """Scenario 03 (enterprise_eng): materialize + first signal of cross_team_dependency_block."""
    await _smoke_check(
        scenario_03,
        "cross_team_dependency_block",
        fresh_db=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
    )
